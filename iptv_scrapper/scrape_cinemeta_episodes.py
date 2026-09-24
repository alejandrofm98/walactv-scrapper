"""Importa episodios Cinemeta y sus textos TMDB antes de que los solicite la app."""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from typing import Any

import requests
from iptv_db.engine import build_url, get_sync_engine, get_sync_session_factory
from sqlalchemy import text

logger = logging.getLogger("cinemeta-episode-scraper")
REQUEST_INTERVAL_SECONDS = 0.3
REQUEST_TIMEOUT_SECONDS = 20


def _clean(value: Any) -> str | None:
    result = str(value).strip() if value is not None else ""
    return result or None


def _number(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


class CinemetaEpisodeScraper:
    """Sincroniza episodios externos sin modificar los capítulos del proveedor."""

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
        self.last_request = 0.0

    def _get_json(self, url: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        for attempt in range(2):
            delay = REQUEST_INTERVAL_SECONDS - (time.monotonic() - self.last_request)
            if delay > 0:
                time.sleep(delay)
            response = self.http_session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            self.last_request = time.monotonic()
            if response.status_code == 429 and attempt == 0:
                retry_after = _number(response.headers.get("Retry-After")) or 5
                time.sleep(min(max(retry_after, 1), 60))
                continue
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("La fuente devolvió un objeto inválido")
            return payload
        raise ValueError("La fuente agotó los reintentos")

    def _tmdb(self, tmdb_id: int, path: str, language: str) -> dict[str, Any]:
        params = {"language": language}
        if not self.read_token:
            params["api_key"] = self.api_key
        try:
            return self._get_json(f"https://api.themoviedb.org/3/tv/{tmdb_id}{path}", params)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return {}
            raise

    def _tmdb_season(self, tmdb_id: int, season: int, language: str) -> dict[str, Any]:
        return self._tmdb(tmdb_id, f"/season/{season}", language)

    def _episode_rows(
        self, imdb_id: str, videos: list[dict[str, Any]], tmdb_id: int | None
    ) -> tuple[list[dict[str, Any]], bool]:
        by_key = {}
        for video in videos:
            season = _number(video.get("season"))
            episode = _number(video.get("episode", video.get("number")))
            if season is not None and episode is not None:
                by_key[(season, episode)] = video

        localized: dict[tuple[int, int], dict[str, dict[str, Any]]] = {}
        had_tmdb_error = False
        if tmdb_id:
            for season in sorted({key[0] for key in by_key}):
                for language, locale in (("es", "es-ES"), ("en", "en-US")):
                    try:
                        payload = self._tmdb_season(tmdb_id, season, locale)
                    except (requests.RequestException, ValueError) as exc:
                        had_tmdb_error = True
                        logger.warning(
                            "TMDB %s temporada %s/%s falló: %s", imdb_id, season, locale, exc
                        )
                        continue
                    for entry in payload.get("episodes", []):
                        if not isinstance(entry, dict):
                            continue
                        number = _number(entry.get("episode_number"))
                        if number is not None:
                            localized.setdefault((season, number), {})[language] = entry

        rows = []
        for (season, episode), video in by_key.items():
            translations = localized.get((season, episode), {})
            es = translations.get("es", {})
            en = translations.get("en", {})
            still_path = _clean(es.get("still_path") or en.get("still_path"))
            rows.append(
                {
                    "imdb_id": imdb_id,
                    "season": season,
                    "episode": episode,
                    "video_id": _clean(video.get("id")) or f"{imdb_id}:{season}:{episode}",
                    "title_es": _clean(es.get("name")),
                    "title_en": _clean(en.get("name") or video.get("name") or video.get("title")),
                    "overview_es": _clean(es.get("overview")),
                    "overview_en": _clean(
                        en.get("overview") or video.get("overview") or video.get("description")
                    ),
                    "thumbnail": (
                        f"https://image.tmdb.org/t/p/w780{still_path}"
                        if still_path
                        else _clean(video.get("thumbnail"))
                    ),
                    "released": _clean(
                        es.get("air_date")
                        or en.get("air_date")
                        or video.get("released")
                        or video.get("firstAired")
                    ),
                }
            )
        return rows, had_tmdb_error

    def run(self, batch_size: int = 25, dry_run: bool = False) -> tuple[int, int, int]:
        """Procesa títulos pendientes o antiguos con commits independientes por serie."""
        with self.session_factory() as db:
            pending = (
                db.execute(
                    text(
                        """
                    SELECT imdb_id, MAX(moviedb_id) AS moviedb_id
                    FROM external_catalog_items
                    WHERE content_type = 'series'
                    GROUP BY imdb_id
                    HAVING (
                        MAX(episodes_synced_at) IS NULL
                        AND (MAX(episodes_checked_at) IS NULL
                             OR MAX(episodes_checked_at) < CURRENT_TIMESTAMP - INTERVAL '1 day')
                    ) OR MAX(episodes_synced_at) < CURRENT_TIMESTAMP - INTERVAL '90 days'
                    ORDER BY MAX(episodes_synced_at) NULLS FIRST,
                             MIN(CASE WHEN catalog_id = 'top' THEN catalog_position ELSE 1000000 END),
                             imdb_id
                    LIMIT :batch_size
                    """
                    ),
                    {"batch_size": batch_size},
                )
                .mappings()
                .all()
            )

            processed = episodes_saved = failed = 0
            for item in pending:
                imdb_id = str(item["imdb_id"])
                try:
                    payload = self._get_json(
                        f"https://v3-cinemeta.strem.io/meta/series/{imdb_id}.json"
                    )
                    meta = payload.get("meta")
                    if not isinstance(meta, dict):
                        raise ValueError("Cinemeta no devolvió una ficha")
                    videos = meta.get("videos") or []
                    if not isinstance(videos, list):
                        raise ValueError("Cinemeta no devolvió episodios válidos")
                    tmdb_id = _number(item["moviedb_id"]) or _number(meta.get("moviedb_id"))
                    rows, had_tmdb_error = self._episode_rows(
                        imdb_id, [video for video in videos if isinstance(video, dict)], tmdb_id
                    )
                    title_es = overview_es = overview_en = None
                    if tmdb_id:
                        for language in ("es-ES", "en-US"):
                            try:
                                details = self._tmdb(tmdb_id, "", language)
                                if language == "es-ES":
                                    title_es = _clean(details.get("name"))
                                    overview_es = _clean(details.get("overview"))
                                else:
                                    overview_en = _clean(details.get("overview"))
                            except (requests.RequestException, ValueError) as exc:
                                had_tmdb_error = True
                                logger.warning("TMDB %s ficha %s falló: %s", imdb_id, language, exc)
                    if not dry_run:
                        if rows:
                            db.execute(
                                text(
                                    """
                                    INSERT INTO external_catalog_episodes (
                                        id, imdb_id, season_number, episode_number, video_id,
                                        title_es, title_en, overview_es, overview_en,
                                        thumbnail, released, updated_at
                                    ) VALUES (
                                        gen_random_uuid(), :imdb_id, :season, :episode, :video_id,
                                        :title_es, :title_en, :overview_es, :overview_en,
                                        :thumbnail, :released, CURRENT_TIMESTAMP
                                    ) ON CONFLICT ON CONSTRAINT uq_external_catalog_episode_identity
                                    DO UPDATE SET
                                        video_id = EXCLUDED.video_id,
                                        title_es = COALESCE(EXCLUDED.title_es, external_catalog_episodes.title_es),
                                        title_en = COALESCE(EXCLUDED.title_en, external_catalog_episodes.title_en),
                                        overview_es = COALESCE(EXCLUDED.overview_es, external_catalog_episodes.overview_es),
                                        overview_en = COALESCE(EXCLUDED.overview_en, external_catalog_episodes.overview_en),
                                        thumbnail = COALESCE(EXCLUDED.thumbnail, external_catalog_episodes.thumbnail),
                                        released = COALESCE(EXCLUDED.released, external_catalog_episodes.released),
                                        updated_at = CURRENT_TIMESTAMP
                                    """
                                ),
                                rows,
                            )
                        db.execute(
                            text(
                                """
                                UPDATE external_catalog_items
                                SET moviedb_id = COALESCE(moviedb_id, :tmdb_id),
                                    title_es = COALESCE(:title_es, title_es),
                                    overview_es = COALESCE(:overview_es, overview_es),
                                    description_en = COALESCE(description_en, :description_en),
                                    logo = COALESCE(:logo, logo),
                                    genres = COALESCE(CAST(:genres AS jsonb), genres),
                                    "cast" = COALESCE(CAST(:cast AS jsonb), "cast"),
                                    episodes_checked_at = CURRENT_TIMESTAMP,
                                    episodes_synced_at = CASE WHEN :complete THEN CURRENT_TIMESTAMP
                                        ELSE episodes_synced_at END
                                WHERE content_type = 'series' AND imdb_id = :imdb_id
                                """
                            ),
                            {
                                "imdb_id": imdb_id,
                                "tmdb_id": tmdb_id,
                                "title_es": title_es,
                                "overview_es": overview_es,
                                "description_en": _clean(meta.get("description")) or overview_en,
                                "logo": _clean(meta.get("logo")),
                                "genres": json.dumps(meta["genres"])
                                if isinstance(meta.get("genres"), list)
                                else None,
                                "cast": json.dumps(meta["cast"])
                                if isinstance(meta.get("cast"), list)
                                else None,
                                "complete": not had_tmdb_error,
                            },
                        )
                        db.commit()
                    processed += 1
                    episodes_saved += len(rows)
                    logger.info(
                        "%s: %s episodios%s",
                        imdb_id,
                        len(rows),
                        " (TMDB incompleto)" if had_tmdb_error else "",
                    )
                except (requests.RequestException, ValueError, TypeError) as exc:
                    db.rollback()
                    if not dry_run:
                        db.execute(
                            text(
                                """
                                UPDATE external_catalog_items
                                SET episodes_checked_at = CURRENT_TIMESTAMP
                                WHERE content_type = 'series' AND imdb_id = :imdb_id
                                """
                            ),
                            {"imdb_id": imdb_id},
                        )
                        db.commit()
                    failed += 1
                    logger.warning("No se pudieron importar episodios de %s: %s", imdb_id, exc)

            if dry_run:
                db.rollback()
        return processed, episodes_saved, failed


def main() -> None:
    parser = argparse.ArgumentParser(description="Importa episodios Cinemeta con sinopsis TMDB")
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size debe ser mayor que cero")
    if not os.getenv("TMDB_API_KEY") and not os.getenv("TMDB_READ_TOKEN"):
        parser.error("Configura TMDB_API_KEY o TMDB_READ_TOKEN")

    url = build_url(
        os.getenv("PG_HOST", "localhost"),
        int(os.getenv("PG_PORT", "5432")),
        os.getenv("PG_DATABASE", "postgres"),
        os.getenv("PG_USER", "postgres"),
        os.getenv("PG_PASSWORD", ""),
        async_driver=False,
    )
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    CinemetaEpisodeScraper(
        get_sync_session_factory(get_sync_engine(url)),
        api_key=os.getenv("TMDB_API_KEY", ""),
        read_token=os.getenv("TMDB_READ_TOKEN", ""),
    ).run(batch_size=args.batch_size, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
