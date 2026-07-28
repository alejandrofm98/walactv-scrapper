#!/usr/bin/env python3
"""
Scraper de tendencias TMDB para WalacTV - Standalone

Uso:
    python iptv_scrapper/scrape_tmdb_trending.py

Variables de entorno requeridas:
    PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
    TMDB_API_KEY, TMDB_READ_TOKEN (opcional)
"""

import logging
import os
import sys
import time
from pathlib import Path

try:
    from dotenv import load_dotenv

    env_path = Path(__file__).parent.parent / "docker" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

import requests
from iptv_db.engine import build_url, get_sync_engine, get_sync_session_factory
from sqlalchemy import text

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
MAX_PAGES = 5
PAGE_SIZE = 20
DELAY_BETWEEN_PAGES = 0.25  # 250ms between pages (well within rate limits)


def _build_session():
    """Build a sync SQLAlchemy session from PG_* env vars."""
    host = os.getenv("PG_HOST", "localhost")
    port = int(os.getenv("PG_PORT", "5432"))
    database = os.getenv("PG_DATABASE", "postgres")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "")
    url = build_url(host, port, database, user, password, async_driver=False)
    engine = get_sync_engine(url)
    Session = get_sync_session_factory(engine)
    return Session()


def _tmdb_headers() -> dict:
    """Build auth headers for TMDB API."""
    if TMDB_READ_TOKEN:
        return {
            "Authorization": f"Bearer {TMDB_READ_TOKEN}",
            "Content-Type": "application/json",
        }
    return {}


def _tmdb_params(extra: dict | None = None) -> dict:
    """Build query params for TMDB API."""
    params = {"language": "es-ES"}
    if not TMDB_READ_TOKEN:
        params["api_key"] = TMDB_API_KEY
    if extra:
        params.update(extra)
    return params


def _fetch_trending(media_type: str, page: int) -> dict:
    """Fetch one page of trending results from TMDB."""
    url = f"{TMDB_BASE_URL}/trending/{media_type}/week"
    headers = _tmdb_headers()
    params = _tmdb_params({"page": page})
    logger.info("GET %s?language=es-ES&page=%d", url, page)
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def scrape_trending():
    """
    Fetch trending movie and TV results from TMDB and upsert into trending_rankings.
    """
    session = _build_session()
    try:
        for media_type in ("movie", "tv"):
            logger.info("=== Procesando trending/%s/week ===", media_type)
            total_inserted = 0

            for page in range(1, MAX_PAGES + 1):
                data = _fetch_trending(media_type, page)
                results = data.get("results", [])
                total_pages = data.get("total_pages", 1)

                if not results:
                    logger.info("  No hay resultados en página %d", page)
                    break

                for idx, item in enumerate(results):
                    tmdb_id = str(item["id"])
                    rank = (page - 1) * PAGE_SIZE + idx + 1

                    session.execute(
                        text("""
                            INSERT INTO trending_rankings
                                (tmdb_id, media_type, rank, trending_window, scraped_at)
                            VALUES (:tmdb_id, :media_type, :rank, 'week', NOW())
                            ON CONFLICT (tmdb_id, media_type, trending_window)
                            DO UPDATE SET
                                rank = EXCLUDED.rank,
                                scraped_at = NOW()
                        """),
                        {
                            "tmdb_id": tmdb_id,
                            "media_type": media_type,
                            "rank": rank,
                        },
                    )
                    total_inserted += 1

                session.commit()
                logger.info(
                    "  Página %d/%d: %d items procesados (acumulado: %d)",
                    page,
                    min(total_pages, MAX_PAGES),
                    len(results),
                    total_inserted,
                )

                if page >= min(total_pages, MAX_PAGES):
                    break

                # Small delay between pages to be respectful
                time.sleep(DELAY_BETWEEN_PAGES)

            logger.info(
                "  Total para %s: %d items upsertados", media_type, total_inserted
            )

        logger.info("Scraping de tendencias TMDB completado exitosamente")

    finally:
        session.close()


def main():
    """Entry point for the trending scraper."""
    logger.info("Iniciando scraper de tendencias TMDB")
    scrape_trending()


if __name__ == "__main__":
    main()
