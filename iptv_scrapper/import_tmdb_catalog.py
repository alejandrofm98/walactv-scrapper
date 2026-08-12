#!/usr/bin/env python3
"""Importa catalogo TMDB adicional sin depender de la playlist IPTV.

El script solo crea catalogo y metadata. La disponibilidad Torrentio se consulta
despues, bajo demanda, desde iptv-api.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import time
from datetime import date
from typing import Any

import requests
from iptv_db.engine import build_url, get_sync_engine, get_sync_session_factory
from iptv_db.models import (
    MovieCatalog,
    MovieMetadata,
    SeriesCatalog,
    SeriesEpisode,
    SeriesMetadata,
)
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

logger = logging.getLogger("walactv.tmdb_catalog")
TMDB_BASE_URL = "https://api.themoviedb.org/3"
SERIES_KEY_RE = re.compile(r"[^a-z0-9]+")


class TmdbCatalogImporter:
    """Importador acotado de peliculas y series populares de TMDB."""

    def __init__(self, session, api_key: str, read_token: str = "") -> None:
        self.session = session
        self.api_key = api_key
        self.http = requests.Session()
        if read_token:
            self.http.headers["Authorization"] = f"Bearer {read_token}"

    def import_media(self, media_type: str, pages: int, max_items: int | None) -> int:
        imported = 0
        seen_ids: set[str] = set()
        for page in range(1, pages + 1):
            payload = self._get(f"/trending/{media_type}/week", {"page": page})
            for item in payload.get("results", []):
                tmdb_id = str(item.get("id", ""))
                if not tmdb_id or tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)
                if media_type == "movie":
                    self._import_movie(tmdb_id)
                else:
                    self._import_series(tmdb_id)
                imported += 1
                if max_items and imported >= max_items:
                    self.session.commit()
                    return imported
            time.sleep(0.25)
        self.session.commit()
        return imported

    def _import_movie(self, tmdb_id: str) -> None:
        details = self._get(f"/movie/{tmdb_id}", {"language": "es-ES"})
        external = self._get(f"/movie/{tmdb_id}/external_ids", {})
        self._upsert_movie_metadata(details, external.get("imdb_id"))

        catalog = self.session.execute(
            select(MovieCatalog).where(MovieCatalog.tmdb_id == tmdb_id)
        ).scalar_one_or_none()
        if catalog is None:
            catalog = MovieCatalog(
                title=details.get("title") or details.get("original_title") or tmdb_id,
                tmdb_id=tmdb_id,
                canonical_key=f"tmdb_{tmdb_id}",
                year=_year(details.get("release_date")),
                countries=[
                    c.get("iso_3166_1")
                    for c in details.get("production_countries", [])
                    if c.get("iso_3166_1")
                ],
                group_normalizado="TORRENTIO",
                logo=None,
                has_iptv_source=False,
                has_torrent_source=False,
            )
            self.session.add(catalog)
        else:
            catalog.title = details.get("title") or catalog.title
            catalog.year = _year(details.get("release_date")) or catalog.year
            catalog.group_normalizado = catalog.group_normalizado or "TORRENTIO"

    def _import_series(self, tmdb_id: str) -> None:
        details = self._get(f"/tv/{tmdb_id}", {"language": "es-ES"})
        external = self._get(f"/tv/{tmdb_id}/external_ids", {})
        imdb_id = external.get("imdb_id")
        self._upsert_series_metadata(details, imdb_id)

        title = details.get("name") or details.get("original_name") or tmdb_id
        series_key = SERIES_KEY_RE.sub("", title.lower()) or f"tmdb{tmdb_id}"
        catalog = self.session.execute(
            select(SeriesCatalog).where(SeriesCatalog.tmdb_id == tmdb_id)
        ).scalar_one_or_none()
        if catalog is None:
            catalog = SeriesCatalog(
                title=title,
                series_key=series_key,
                tmdb_id=tmdb_id,
                canonical_key=f"tmdb_{tmdb_id}",
                year=_year(details.get("first_air_date")),
                countries=details.get("origin_country") or [],
                group_normalizado="TORRENTIO",
                has_iptv_source=False,
                has_torrent_source=False,
            )
            self.session.add(catalog)
            self.session.flush()
        else:
            catalog.title = title
            catalog.group_normalizado = catalog.group_normalizado or "TORRENTIO"

        for season in details.get("seasons", []):
            season_number = season.get("season_number")
            if season_number is None:
                continue
            season_details = self._get(
                f"/tv/{tmdb_id}/season/{season_number}", {"language": "es-ES"}
            )
            for episode in season_details.get("episodes", []):
                self._upsert_episode(catalog.id, episode)

    def _upsert_movie_metadata(self, details: dict[str, Any], imdb_id: str | None) -> None:
        tmdb_id = str(details["id"])
        values = {
            "tmdb_id": tmdb_id,
            "imdb_id": imdb_id,
            "title": details.get("title"),
            "original_title": details.get("original_title"),
            "overview_es": details.get("overview"),
            "genres": [g.get("name") for g in details.get("genres", []) if g.get("name")],
            "vote_average": details.get("vote_average"),
            "vote_count": details.get("vote_count"),
            "poster_path": details.get("poster_path"),
            "backdrop_path": details.get("backdrop_path"),
            "release_date": _date(details.get("release_date")),
            "year": _year(details.get("release_date")),
            "runtime_minutes": details.get("runtime"),
            "tagline": details.get("tagline"),
            "popularity": details.get("popularity"),
            "status": details.get("status"),
            "tmdb_data": details,
        }
        stmt = pg_insert(MovieMetadata).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=[MovieMetadata.tmdb_id],
            set_={key: getattr(stmt.excluded, key) for key in values if key != "tmdb_id"},
        )
        self.session.execute(stmt)

    def _upsert_series_metadata(self, details: dict[str, Any], imdb_id: str | None) -> None:
        tmdb_id = str(details["id"])
        values = {
            "tmdb_id": tmdb_id,
            "imdb_id": imdb_id,
            "title": details.get("name"),
            "original_title": details.get("original_name"),
            "overview_es": details.get("overview"),
            "genres": [g.get("name") for g in details.get("genres", []) if g.get("name")],
            "vote_average": details.get("vote_average"),
            "vote_count": details.get("vote_count"),
            "poster_path": details.get("poster_path"),
            "backdrop_path": details.get("backdrop_path"),
            "release_date": _date(details.get("first_air_date")),
            "year": _year(details.get("first_air_date")),
            "tagline": details.get("tagline"),
            "popularity": details.get("popularity"),
            "status": details.get("status"),
            "tmdb_data": details,
        }
        stmt = pg_insert(SeriesMetadata).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=[SeriesMetadata.tmdb_id],
            set_={key: getattr(stmt.excluded, key) for key in values if key != "tmdb_id"},
        )
        self.session.execute(stmt)

    def _upsert_episode(self, catalog_id, episode: dict[str, Any]) -> None:
        values = {
            "catalog_id": catalog_id,
            "season_number": episode.get("season_number", 0),
            "episode_number": episode.get("episode_number", 0),
            "title": episode.get("name"),
            "overview": episode.get("overview"),
            "air_date": _date(episode.get("air_date")),
            "still_path": episode.get("still_path"),
            "runtime": episode.get("runtime"),
            "vote_average": episode.get("vote_average"),
            "vote_count": episode.get("vote_count"),
            "episode_type": episode.get("episode_type"),
            "tmdb_checked": True,
        }
        stmt = pg_insert(SeriesEpisode).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=[
                SeriesEpisode.catalog_id,
                SeriesEpisode.season_number,
                SeriesEpisode.episode_number,
            ],
            set_={key: getattr(stmt.excluded, key) for key in values if key != "catalog_id"},
        )
        self.session.execute(stmt)

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        query = {"api_key": self.api_key, **params}
        response = self.http.get(f"{TMDB_BASE_URL}{path}", params=query, timeout=20)
        response.raise_for_status()
        return response.json()


def _year(value: str | None) -> int | None:
    if value and len(value) >= 4 and value[:4].isdigit():
        return int(value[:4])
    return None


def _date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Importa catalogo TMDB para Torrentio")
    parser.add_argument("--media-type", choices=("movie", "tv", "both"), default="both")
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--max-items", type=int)
    args = parser.parse_args()

    api_key = os.getenv("TMDB_API_KEY")
    if not api_key:
        raise SystemExit("TMDB_API_KEY no esta configurada")
    url = build_url(
        os.getenv("PG_HOST", "localhost"),
        int(os.getenv("PG_PORT", "5432")),
        os.getenv("PG_DATABASE", "postgres"),
        os.getenv("PG_USER", "postgres"),
        os.getenv("PG_PASSWORD", ""),
        async_driver=False,
    )
    Session = get_sync_session_factory(get_sync_engine(url))
    media_types = ("movie", "tv") if args.media_type == "both" else (args.media_type,)
    with Session() as session:
        importer = TmdbCatalogImporter(session, api_key, os.getenv("TMDB_READ_TOKEN", ""))
        total = sum(
            importer.import_media(media_type, args.pages, args.max_items)
            for media_type in media_types
        )
    logger.info("Contenido TMDB importado: %d", total)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
