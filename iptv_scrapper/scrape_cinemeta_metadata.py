"""Guarda en PostgreSQL las sinopsis españolas TMDB del catálogo Cinemeta importado."""

from __future__ import annotations

import argparse
import logging
import os
import time
from typing import Any

import requests
from iptv_db.engine import build_url, get_sync_engine, get_sync_session_factory
from sqlalchemy import text

TMDB_BASE_URL = "https://api.themoviedb.org/3"
REQUEST_TIMEOUT_SECONDS = 15
RETRY_DELAY_SECONDS = 5
RATE_LIMIT_REQUESTS = 35
RATE_LIMIT_WINDOW_SECONDS = 10

logger = logging.getLogger("cinemeta-tmdb-scraper")


class CinemetaMetadataScraper:
    """Enriquece las fichas Cinemeta importadas por el scraper, sin depender de la app."""

    def __init__(
        self,
        session_factory: Any,
        api_key: str = "",
        read_token: str = "",
        http_session: requests.Session | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.api_key = api_key.strip()
        self.read_token = read_token.strip()
        self.http_session = http_session or requests.Session()
        if self.read_token:
            self.http_session.headers["Authorization"] = f"Bearer {self.read_token}"
        self.http_session.headers["Accept"] = "application/json"
        self.window_started = time.monotonic()
        self.request_count = 0

    def _rate_limit(self) -> None:
        """Mantiene el scraper por debajo del límite sostenido de TMDB."""
        elapsed = time.monotonic() - self.window_started
        if self.request_count >= RATE_LIMIT_REQUESTS:
            time.sleep(max(0, RATE_LIMIT_WINDOW_SECONDS - elapsed))
            self.window_started = time.monotonic()
            self.request_count = 0
        self.request_count += 1

    def _fetch_spanish(self, content_type: str, moviedb_id: int) -> dict[str, str | None]:
        resource = "tv" if content_type == "series" else "movie"
        params: dict[str, str] = {"language": "es-ES"}
        if not self.read_token:
            params["api_key"] = self.api_key

        url = f"{TMDB_BASE_URL}/{resource}/{moviedb_id}"
        for attempt in range(2):
            self._rate_limit()
            response = self.http_session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code == 429 and attempt == 0:
                try:
                    retry_after = int(response.headers.get("Retry-After", RETRY_DELAY_SECONDS))
                except (TypeError, ValueError):
                    retry_after = RETRY_DELAY_SECONDS
                time.sleep(max(1, min(retry_after, 60)))
                continue
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("TMDB devolvió una respuesta inválida")
            title = payload.get("title") or payload.get("name")
            overview = payload.get("overview")
            return {
                "title_es": str(title).strip() if title and str(title).strip() else None,
                "overview_es": (
                    str(overview).strip() if overview and str(overview).strip() else None
                ),
            }
        return {"title_es": None, "overview_es": None}

    def run(self, batch_size: int = 100, dry_run: bool = False) -> tuple[int, int, int]:
        """Procesa fichas pendientes y devuelve procesadas, traducidas y fallidas."""
        with self.session_factory() as db:
            rows = (
                db.execute(
                    text(
                        """
                    SELECT c.content_type, c.imdb_id, MAX(c.moviedb_id) AS moviedb_id
                    FROM external_catalog_items AS c
                    WHERE c.moviedb_id IS NOT NULL AND c.moviedb_id > 0
                      AND NOT EXISTS (
                          SELECT 1
                          FROM external_catalog_items AS localized
                          WHERE localized.content_type = c.content_type
                            AND localized.imdb_id = c.imdb_id
                            AND localized.overview_es IS NOT NULL
                            AND localized.overview_es <> ''
                      )
                    GROUP BY c.content_type, c.imdb_id
                    ORDER BY MIN(c.updated_at), c.content_type, c.imdb_id
                    LIMIT :batch_size
                    """
                    ),
                    {"batch_size": batch_size},
                )
                .mappings()
                .all()
            )

            processed = translated = failed = 0
            for row in rows:
                content_type = str(row["content_type"])
                imdb_id = str(row["imdb_id"])
                try:
                    spanish = self._fetch_spanish(content_type, int(row["moviedb_id"]))
                    processed += 1
                    if spanish["overview_es"]:
                        translated += 1
                    if not dry_run:
                        db.execute(
                            text(
                                """
                                UPDATE external_catalog_items
                                SET title_es = COALESCE(:title_es, title_es),
                                    overview_es = COALESCE(:overview_es, overview_es),
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE content_type = :content_type AND imdb_id = :imdb_id
                                """
                            ),
                            {
                                "title_es": spanish["title_es"],
                                "overview_es": spanish["overview_es"],
                                "content_type": content_type,
                                "imdb_id": imdb_id,
                            },
                        )
                    logger.info(
                        "Cinemeta %s: %s",
                        imdb_id,
                        "sinopsis ES encontrada" if spanish["overview_es"] else "sin sinopsis ES",
                    )
                except (requests.RequestException, ValueError, TypeError) as exc:
                    failed += 1
                    if not dry_run:
                        db.execute(
                            text(
                                """
                                UPDATE external_catalog_items
                                SET updated_at = CURRENT_TIMESTAMP
                                WHERE content_type = :content_type AND imdb_id = :imdb_id
                                """
                            ),
                            {"content_type": content_type, "imdb_id": imdb_id},
                        )
                    logger.warning("Error TMDB para Cinemeta %s (%s)", imdb_id, type(exc).__name__)

            if not dry_run:
                db.commit()
            else:
                db.rollback()

        logger.info(
            "Scraper Cinemeta/TMDB: %s procesadas, %s con sinopsis ES, %s errores",
            processed,
            translated,
            failed,
        )
        return processed, translated, failed


def _create_session_factory() -> Any:
    url = build_url(
        os.getenv("PG_HOST", "localhost"),
        int(os.getenv("PG_PORT", "5432")),
        os.getenv("PG_DATABASE", "postgres"),
        os.getenv("PG_USER", "postgres"),
        os.getenv("PG_PASSWORD", ""),
        async_driver=False,
    )
    return get_sync_session_factory(get_sync_engine(url))


def main() -> None:
    parser = argparse.ArgumentParser(description="Enriquece Cinemeta con sinopsis TMDB en español")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size debe ser mayor que cero")

    api_key = os.getenv("TMDB_API_KEY", "")
    read_token = os.getenv("TMDB_READ_TOKEN", "")
    if not api_key and not read_token:
        parser.error("Configura TMDB_API_KEY o TMDB_READ_TOKEN")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    scraper = CinemetaMetadataScraper(
        _create_session_factory(), api_key=api_key, read_token=read_token
    )
    scraper.run(batch_size=args.batch_size, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
