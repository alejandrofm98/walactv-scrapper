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

    def _get_json(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        if not self.read_token:
            params["api_key"] = self.api_key
        for attempt in range(2):
            self._rate_limit()
            response = self.http_session.get(
                f"{TMDB_BASE_URL}/{path}", params=params, timeout=REQUEST_TIMEOUT_SECONDS
            )
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
            return payload
        raise ValueError("TMDB agotó los reintentos")

    def _find_tmdb_id(self, content_type: str, imdb_id: str) -> int | None:
        payload = self._get_json(f"find/{imdb_id}", {"external_source": "imdb_id"})
        key = "tv_results" if content_type == "series" else "movie_results"
        matches = payload.get(key) or []
        if not isinstance(matches, list) or not matches:
            return None
        try:
            return int(matches[0]["id"])
        except (KeyError, TypeError, ValueError):
            return None

    def _fetch_logo(self, content_type: str, moviedb_id: int) -> str | None:
        resource = "tv" if content_type == "series" else "movie"
        payload = self._get_json(
            f"{resource}/{moviedb_id}/images", {"include_image_language": "es,en,null"}
        )
        logos = payload.get("logos") or []
        if not isinstance(logos, list):
            return None
        candidates = [
            logo for logo in logos
            if isinstance(logo, dict)
            and isinstance(logo.get("file_path"), str)
            and not logo["file_path"].endswith(".svg")
        ]
        if not candidates:
            return None
        priority = {"es": 3, "en": 2, None: 1}
        best = max(
            candidates,
            key=lambda logo: (
                priority.get(logo.get("iso_639_1"), 0),
                logo.get("vote_average") or 0,
                logo.get("vote_count") or 0,
            ),
        )
        return f"https://image.tmdb.org/t/p/w500{best['file_path']}"

    def _fetch_spanish(self, content_type: str, moviedb_id: int) -> dict[str, str | None]:
        resource = "tv" if content_type == "series" else "movie"
        payload = self._get_json(f"{resource}/{moviedb_id}", {"language": "es-ES"})
        if not str(payload.get("overview") or "").strip():
            mexican = self._get_json(f"{resource}/{moviedb_id}", {"language": "es-MX"})
            if str(mexican.get("overview") or "").strip():
                payload = mexican
        title = payload.get("title") or payload.get("name")
        overview = payload.get("overview")
        return {
            "title_es": str(title).strip() if title and str(title).strip() else None,
            "overview_es": str(overview).strip() if overview and str(overview).strip() else None,
        }

    def run(
        self, batch_size: int = 100, dry_run: bool = False, imdb_id: str | None = None
    ) -> tuple[int, int, int]:
        """Procesa fichas pendientes y devuelve procesadas, traducidas y fallidas."""
        with self.session_factory() as db:
            rows = (
                db.execute(
                    text(
                        """
                    SELECT c.content_type, c.imdb_id, MAX(c.moviedb_id) AS moviedb_id,
                           BOOL_OR(NULLIF(c.overview_es, '') IS NOT NULL) AS has_overview_es,
                           MAX(NULLIF(c.logo, '')) AS existing_logo
                    FROM external_catalog_items AS c
                    WHERE (:imdb_id IS NULL OR c.imdb_id = :imdb_id)
                    GROUP BY c.content_type, c.imdb_id
                    HAVING :imdb_id IS NOT NULL OR ((MAX(c.localized_checked_at) IS NULL
                        OR MAX(c.localized_checked_at) < CURRENT_TIMESTAMP - INTERVAL '30 days')
                       AND (MAX(c.moviedb_id) IS NULL
                            OR NOT BOOL_OR(NULLIF(c.overview_es, '') IS NOT NULL)
                            OR MAX(NULLIF(c.logo, '')) IS NULL
                            OR MAX(NULLIF(c.logo, '')) NOT LIKE 'https://image.tmdb.org/t/p/%'))
                    ORDER BY MAX(c.localized_checked_at) NULLS FIRST,
                             c.content_type, c.imdb_id
                    LIMIT :batch_size
                    """
                    ),
                    {"batch_size": batch_size, "imdb_id": imdb_id},
                )
                .mappings()
                .all()
            )

            processed = translated = failed = 0
            for row in rows:
                content_type = str(row["content_type"])
                imdb_id = str(row["imdb_id"])
                try:
                    moviedb_id = row["moviedb_id"] or self._find_tmdb_id(content_type, imdb_id)
                    spanish = {"title_es": None, "overview_es": None}
                    logo = None
                    if moviedb_id and not row["has_overview_es"]:
                        spanish = self._fetch_spanish(content_type, int(moviedb_id))
                    if moviedb_id and not str(row["existing_logo"] or "").startswith(
                        "https://image.tmdb.org/t/p/"
                    ):
                        try:
                            logo = self._fetch_logo(content_type, int(moviedb_id))
                        except (requests.RequestException, ValueError, TypeError) as exc:
                            logger.warning("Logo TMDB %s falló: %s", imdb_id, type(exc).__name__)
                    processed += 1
                    if spanish["overview_es"]:
                        translated += 1
                    if not dry_run:
                        db.execute(
                            text(
                                """
                                UPDATE external_catalog_items
                                SET moviedb_id = COALESCE(moviedb_id, :moviedb_id),
                                    episodes_checked_at = CASE
                                        WHEN moviedb_id IS NULL AND :moviedb_id IS NOT NULL THEN NULL
                                        ELSE episodes_checked_at END,
                                    episodes_synced_at = CASE
                                        WHEN moviedb_id IS NULL AND :moviedb_id IS NOT NULL THEN NULL
                                        ELSE episodes_synced_at END,
                                    title_es = COALESCE(:title_es, title_es),
                                    overview_es = COALESCE(:overview_es, overview_es),
                                    logo = COALESCE(:logo, logo, :existing_logo),
                                    localized_checked_at = CURRENT_TIMESTAMP,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE content_type = :content_type AND imdb_id = :imdb_id
                                """
                            ),
                            {
                                "title_es": spanish["title_es"],
                                "overview_es": spanish["overview_es"],
                                "moviedb_id": moviedb_id,
                                "logo": logo,
                                "existing_logo": row["existing_logo"],
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
                                SET localized_checked_at = CURRENT_TIMESTAMP - INTERVAL '29 days',
                                    updated_at = CURRENT_TIMESTAMP
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
    parser.add_argument("--imdb-id", help="Enriquecer una ficha concreta sin esperar al barrido")
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size debe ser mayor que cero")
    if args.imdb_id and not (args.imdb_id.startswith("tt") and args.imdb_id[2:].isdigit()):
        parser.error("--imdb-id debe tener formato tt1234567")

    api_key = os.getenv("TMDB_API_KEY", "")
    read_token = os.getenv("TMDB_READ_TOKEN", "")
    if not api_key and not read_token:
        parser.error("Configura TMDB_API_KEY o TMDB_READ_TOKEN")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    scraper = CinemetaMetadataScraper(
        _create_session_factory(), api_key=api_key, read_token=read_token
    )
    scraper.run(batch_size=args.batch_size, dry_run=args.dry_run, imdb_id=args.imdb_id)


if __name__ == "__main__":
    main()
