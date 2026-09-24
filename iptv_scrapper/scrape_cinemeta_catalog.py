"""Importa en PostgreSQL los catálogos públicos que Cinemeta declara en su manifiesto."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from collections.abc import Iterator
from typing import Any

import requests
from iptv_db.engine import build_url, get_sync_engine, get_sync_session_factory
from sqlalchemy import text

CINEMETA_BASE_URL = "https://v3-cinemeta.strem.io"
REQUEST_TIMEOUT_SECONDS = 20
REQUEST_INTERVAL_SECONDS = 0.15
logger = logging.getLogger("cinemeta-catalog-scraper")


def _slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", value).strip("_")[:40]


def _string_list(value: Any) -> str | None:
    if not isinstance(value, list):
        return None
    return json.dumps([str(item) for item in value if item])


def _catalog_variants(manifest: dict[str, Any]) -> Iterator[tuple[str, str, str, str | None]]:
    """Expande los catálogos movie/series que admiten paginación y sus filtros."""
    for catalog in manifest.get("catalogs", []):
        if not isinstance(catalog, dict):
            continue
        content_type = catalog.get("type")
        catalog_id = catalog.get("id")
        if content_type not in ("movie", "series") or not catalog_id:
            continue
        extras = catalog.get("extraSupported", [])
        if "skip" not in extras:
            continue

        required = {item.get("name") for item in catalog.get("extra", []) if item.get("isRequired")}
        genres = catalog.get("genres", [])
        if "genre" in required:
            values = genres
        elif "genre" in extras:
            values = [None, *genres]
        else:
            values = [None]

        for value in values:
            key = str(catalog_id) if value is None else f"{catalog_id}_{_slug(str(value))}"
            yield str(content_type), str(catalog_id), key, str(value) if value is not None else None


class CinemetaCatalogScraper:
    """Recorre Cinemeta directamente; no depende de tráfico ni solicitudes de usuarios."""

    def __init__(self, session_factory: Any, http_session: requests.Session | None = None) -> None:
        self.session_factory = session_factory
        self.http_session = http_session or requests.Session()
        self.http_session.headers["Accept"] = "application/json"
        self.last_request = 0.0

    def _get_json(self, path: str) -> dict[str, Any]:
        delay = REQUEST_INTERVAL_SECONDS - (time.monotonic() - self.last_request)
        if delay > 0:
            time.sleep(delay)
        response = self.http_session.get(
            f"{CINEMETA_BASE_URL}{path}", timeout=REQUEST_TIMEOUT_SECONDS
        )
        self.last_request = time.monotonic()
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Cinemeta devolvió una respuesta inválida")
        return payload

    def _catalog_page(
        self, content_type: str, source_id: str, skip: int, genre: str | None
    ) -> list[dict[str, Any]]:
        path = f"/catalog/{content_type}/{source_id}"
        if genre is not None:
            path += f"/genre={requests.utils.quote(genre, safe='')}"
        if skip:
            path += f"/skip={skip}"
        try:
            metas = self._get_json(f"{path}.json").get("metas", [])
        except requests.HTTPError as exc:
            # Cinemeta devuelve 404 (no una lista vacía) al agotar un filtro.
            if skip > 0 and exc.response is not None and exc.response.status_code == 404:
                return []
            raise
        if not isinstance(metas, list):
            raise ValueError(f"Cinemeta devolvió una página inválida: {content_type}/{source_id}")
        return [item for item in metas if isinstance(item, dict) and item.get("id")]

    def _get_meta(self, content_type: str, imdb_id: str) -> dict[str, Any]:
        payload = self._get_json(f"/meta/{content_type}/{imdb_id}.json")
        meta = payload.get("meta")
        return meta if isinstance(meta, dict) else {}

    def _upsert_page(
        self,
        db: Any,
        content_type: str,
        catalog_id: str,
        skip: int,
        items: list[dict[str, Any]],
        metas: dict[str, dict[str, Any]],
    ) -> int:
        rows = []
        for offset, item in enumerate(items):
            imdb_id = str(item.get("id") or "")
            if not imdb_id.startswith("tt"):
                continue
            meta = metas.get(imdb_id, {})
            moviedb_id = meta.get("moviedb_id") or item.get("moviedb_id")
            try:
                moviedb_id = int(moviedb_id) if moviedb_id is not None else None
            except (TypeError, ValueError):
                moviedb_id = None
            release = str(item.get("releaseInfo") or item.get("year") or "")
            year_match = re.search(r"\b(19|20)\d{2}\b", release)
            try:
                rating = float(item.get("imdbRating")) if item.get("imdbRating") else None
            except (TypeError, ValueError):
                rating = None
            rows.append(
                {
                    "content_type": content_type,
                    "catalog_id": catalog_id,
                    "catalog_position": skip + offset,
                    "imdb_id": imdb_id,
                    "moviedb_id": moviedb_id,
                    "title": item.get("name") or item.get("title") or meta.get("name"),
                    "description_en": item.get("description") or meta.get("description"),
                    "poster": item.get("poster") or meta.get("poster"),
                    "backdrop": item.get("background") or meta.get("background"),
                    "logo": item.get("logo") or meta.get("logo"),
                    "genres": _string_list(item.get("genres") or meta.get("genres")),
                    "cast": _string_list(item.get("cast") or meta.get("cast")),
                    "rating": rating,
                    "year": int(year_match.group(0)) if year_match else None,
                }
            )
        if not rows:
            return 0
        db.execute(
            text(
                """
                INSERT INTO external_catalog_items (
                    id, content_type, catalog_id, catalog_position, imdb_id, moviedb_id,
                    title, description_en, poster, backdrop, logo, genres, "cast", rating, year,
                    imported_at, last_seen_at, updated_at
                ) VALUES (
                    gen_random_uuid(), :content_type, :catalog_id, :catalog_position,
                    :imdb_id, :moviedb_id,
                    :title, :description_en, :poster, :backdrop, :logo,
                    CAST(:genres AS jsonb), CAST(:cast AS jsonb), :rating, :year,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON CONFLICT ON CONSTRAINT uq_external_catalog_items_identity DO UPDATE SET
                    catalog_position = EXCLUDED.catalog_position,
                    moviedb_id = COALESCE(EXCLUDED.moviedb_id, external_catalog_items.moviedb_id),
                    episodes_checked_at = CASE
                        WHEN external_catalog_items.moviedb_id IS NULL AND EXCLUDED.moviedb_id IS NOT NULL
                            THEN NULL
                        ELSE external_catalog_items.episodes_checked_at
                    END,
                    episodes_synced_at = CASE
                        WHEN external_catalog_items.moviedb_id IS NULL AND EXCLUDED.moviedb_id IS NOT NULL
                            THEN NULL
                        ELSE external_catalog_items.episodes_synced_at
                    END,
                    title = COALESCE(EXCLUDED.title, external_catalog_items.title),
                    description_en = COALESCE(EXCLUDED.description_en, external_catalog_items.description_en),
                    poster = COALESCE(EXCLUDED.poster, external_catalog_items.poster),
                    backdrop = COALESCE(EXCLUDED.backdrop, external_catalog_items.backdrop),
                    logo = COALESCE(external_catalog_items.logo, EXCLUDED.logo),
                    genres = COALESCE(EXCLUDED.genres, external_catalog_items.genres),
                    "cast" = COALESCE(EXCLUDED."cast", external_catalog_items."cast"),
                    rating = COALESCE(EXCLUDED.rating, external_catalog_items.rating),
                    year = COALESCE(EXCLUDED.year, external_catalog_items.year),
                    last_seen_at = CURRENT_TIMESTAMP
                """
            ),
            rows,
        )
        return len(rows)

    def run(self, dry_run: bool = False) -> tuple[int, int, int]:
        """Importa todas las páginas de cada catálogo/filter declarado por Cinemeta."""
        manifest = self._get_json("/manifest.json")
        variants = list(_catalog_variants(manifest))
        page_count = item_count = failed_count = 0
        with self.session_factory() as db:
            metadata_cache: dict[tuple[str, str], dict[str, Any]] = {}
            for content_type, source_id, storage_id, genre in variants:
                skip = 0
                while True:
                    try:
                        items = self._catalog_page(content_type, source_id, skip, genre)
                        if not items:
                            break
                        metas = {}
                        ids = [str(item.get("id")) for item in items if item.get("id")]
                        existing = db.execute(
                            text(
                                """
                                SELECT imdb_id, BOOL_OR(moviedb_id IS NOT NULL) AS has_moviedb_id,
                                       BOOL_OR(updated_at > CURRENT_TIMESTAMP - INTERVAL '30 days')
                                           AS recently_checked
                                FROM external_catalog_items
                                WHERE content_type = :content_type AND imdb_id = ANY(:imdb_ids)
                                GROUP BY imdb_id
                                """
                            ),
                            {"content_type": content_type, "imdb_ids": ids},
                        ).mappings()
                        known = {str(row["imdb_id"]): row for row in existing}
                        for item in items:
                            imdb_id = str(item.get("id") or "")
                            cache_key = (content_type, imdb_id)
                            known_row = known.get(imdb_id)
                            has_item_moviedb_id = bool(item.get("moviedb_id"))
                            recently_checked = bool(known_row and known_row["recently_checked"])
                            needs_meta = not known_row or (
                                not known_row["has_moviedb_id"] and not recently_checked
                            )
                            if (
                                imdb_id.startswith("tt")
                                and not has_item_moviedb_id
                                and needs_meta
                                and cache_key not in metadata_cache
                            ):
                                try:
                                    metadata_cache[cache_key] = self._get_meta(
                                        content_type, imdb_id
                                    )
                                except (requests.RequestException, ValueError) as exc:
                                    logger.warning(
                                        "Ficha Cinemeta %s falló (%s)", imdb_id, type(exc).__name__
                                    )
                                    metadata_cache[cache_key] = {}
                            if cache_key in metadata_cache:
                                metas[imdb_id] = metadata_cache[cache_key]
                        added = self._upsert_page(db, content_type, storage_id, skip, items, metas)
                        page_count += 1
                        item_count += added
                        if not dry_run:
                            db.commit()
                        logger.info(
                            "Cinemeta %s/%s%s skip=%s: %s fichas",
                            content_type,
                            source_id,
                            f" género={genre}" if genre else "",
                            skip,
                            added,
                        )
                        skip += len(items)
                    except (requests.RequestException, ValueError) as exc:
                        failed_count += 1
                        db.rollback()
                        logger.error(
                            "Catálogo Cinemeta %s/%s%s skip=%s falló (%s)",
                            content_type,
                            source_id,
                            f" género={genre}" if genre else "",
                            skip,
                            type(exc).__name__,
                        )
                        break
            if dry_run:
                db.rollback()
        logger.info(
            "Importación Cinemeta finalizada: %s catálogos, %s páginas, %s fichas, %s errores",
            len(variants),
            page_count,
            item_count,
            failed_count,
        )
        return page_count, item_count, failed_count


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
    parser = argparse.ArgumentParser(
        description="Importa catálogos Cinemeta completos en PostgreSQL"
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    CinemetaCatalogScraper(_create_session_factory()).run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
