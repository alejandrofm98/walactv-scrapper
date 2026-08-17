#!/usr/bin/env python3
"""Clasifica el catalogo consultando Torrentio una vez por titulo.

Al anadir contenido, el scrapper deja has_torrent_source=False. Este script
consulta Torrentio por imdb_id y persiste en la BD:
  - has_torrent_source: si el titulo tiene al menos un stream torrent valido
  - torrent_languages: idiomas detectados en los streams (["ES", "EN", ...])
  - torrent_source_checked_at: timestamp de la ultima consulta

Se ejecuta periodicamente (cron) para refrescar titulos antiguos.

Uso:
    python iptv_scrapper/classify_torrentio.py --batch-size 50
    python iptv_scrapper/classify_torrentio.py --refresh-days 7
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select

try:
    from dotenv import load_dotenv

    env_path = __import__("pathlib").Path(__file__).parent.parent / "docker" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

from iptv_db.engine import build_url, get_sync_engine, get_sync_session_factory
from iptv_db.models import (
    MovieCatalog,
    MovieMetadata,
    SeriesCatalog,
    SeriesEpisode,
    SeriesMetadata,
)

from iptv_scrapper.torrentio import TorrentioClient

logger = logging.getLogger("torrentio-classify")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stderr)
_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
)
logger.addHandler(_handler)

# Marcadores de idioma que Torrentio inserta en el titulo del stream.
_LANGUAGE_CODES = {"🇪🇸": "ES", "🇬🇧": "EN", "🇯🇵": "JP"}
_EXCLUDED_LANGUAGE_MARKERS = ("🇲🇽", "latino")
_FOREIGN_FLAGS = ("🇮🇹", "🇵🇹", "🇷🇺", "🇫🇷", "🇩🇪", "🇵🇱", "🇨🇳", "🇯🇵")


@dataclass
class Classification:
    """Resultado de clasificar un titulo."""

    has_torrent: bool
    languages: list[str]


def detect_languages(title: str) -> list[str] | None:
    """Detecta los idiomas presentes en el titulo de un stream de Torrentio.

    Devuelve None si el stream no es util (latino/idioma extranjero sin codigo).
    """
    lowered = title.lower()
    if any(marker in lowered for marker in _EXCLUDED_LANGUAGE_MARKERS):
        return None
    foreign_flags = any(marker in title for marker in _FOREIGN_FLAGS)
    explicit = [marker for marker in _LANGUAGE_CODES if marker in title]
    # Formato [ES] / [EN] / [JP] usado por algunos providers.
    bracketed = re.findall(r"\[(ES|EN|JP)\]", title, re.IGNORECASE)
    if foreign_flags and not explicit and not bracketed:
        return None
    if "🇪🇸" in title:
        return ["ES"]
    if "🇬🇧" in title:
        return ["EN"]
    if "🇯🇵" in title or re.search(r"\b(japanese|japonesa?|japon(?:es|és)?)\b", lowered):
        return ["JP"]
    if "日本語" in title or "日本" in title:
        return ["JP"]
    if re.search(r"\b(spanish|castellano)\b", lowered):
        return ["ES"]
    if re.search(r"\benglish\b", lowered):
        return ["EN"]
    if explicit:
        return [_LANGUAGE_CODES[marker] for marker in explicit]
    if bracketed:
        return [code.upper() for code in bracketed]
    return ["EN"]


def classify_streams(streams: list[dict[str, Any]]) -> Classification:
    """Clasifica una lista de streams de Torrentio en idiomas y disponibilidad."""
    languages: set[str] = set()
    has_valid = False
    for stream in streams:
        info_hash = str(stream.get("infoHash") or "").strip()
        if not re.fullmatch(r"[a-fA-F0-9]{40}", info_hash):
            continue
        title = str(stream.get("title") or "").strip()
        detected = detect_languages(title)
        if detected is None:
            continue
        has_valid = True
        languages.update(detected)
    return Classification(has_torrent=has_valid, languages=sorted(languages))


def _catalog_rows(session, refresh_days: int, batch_size: int) -> list[tuple[str, str, Any]]:
    """Devuelve (kind, imdb_id, row) del catalogo pendiente de clasificar."""
    cutoff = datetime.now(UTC) - timedelta(days=refresh_days)
    rows: list[tuple[str, str, Any]] = []

    movies = session.execute(
        select(MovieCatalog.id, MovieMetadata.imdb_id)
        .join(MovieMetadata, MovieMetadata.tmdb_id == MovieCatalog.tmdb_id)
        .where(
            MovieMetadata.imdb_id.is_not(None),
            (MovieCatalog.torrent_source_checked_at.is_(None))
            | (MovieCatalog.torrent_source_checked_at < cutoff),
        )
        .order_by(MovieCatalog.torrent_source_checked_at.asc().nullsfirst())
        .limit(batch_size)
    ).all()
    for catalog_id, imdb_id in movies:
        rows.append(("movie", imdb_id, catalog_id))

    series = session.execute(
        select(SeriesCatalog.id, SeriesMetadata.imdb_id)
        .join(SeriesMetadata, SeriesMetadata.tmdb_id == SeriesCatalog.tmdb_id)
        .where(
            SeriesMetadata.imdb_id.is_not(None),
            (SeriesCatalog.torrent_source_checked_at.is_(None))
            | (SeriesCatalog.torrent_source_checked_at < cutoff),
        )
        .order_by(SeriesCatalog.torrent_source_checked_at.asc().nullsfirst())
        .limit(batch_size)
    ).all()
    for catalog_id, imdb_id in series:
        rows.append(("series", imdb_id, catalog_id))

    return rows


def _apply_movie(session, catalog_id: Any, classification: Classification) -> None:
    session.execute(
        MovieCatalog.__table__.update()
        .where(MovieCatalog.id == catalog_id)
        .values(
            has_torrent_source=classification.has_torrent,
            torrent_languages=classification.languages,
            torrent_source_checked_at=datetime.now(UTC),
        )
    )


def _apply_series(session, catalog_id: Any, classification: Classification) -> None:
    session.execute(
        SeriesCatalog.__table__.update()
        .where(SeriesCatalog.id == catalog_id)
        .values(
            has_torrent_source=classification.has_torrent,
            torrent_languages=classification.languages,
            torrent_source_checked_at=datetime.now(UTC),
        )
    )
    # Los episodios heredan la clasificacion del catalogo padre.
    session.execute(
        SeriesEpisode.__table__.update()
        .where(SeriesEpisode.catalog_id == catalog_id)
        .values(
            has_torrent_source=classification.has_torrent,
            torrent_languages=classification.languages,
            torrent_source_checked_at=datetime.now(UTC),
        )
    )


def run_classify(batch_size: int, refresh_days: int, dry_run: bool) -> dict[str, Any]:
    """Clasifica un lote del catalogo. Devuelve estadisticas."""
    stats: dict[str, Any] = {"checked": 0, "with_torrent": 0, "errors": 0}
    start = time.time()

    url = os.getenv("DATABASE_URL")
    if url:
        engine = get_sync_engine(url)
    else:
        engine = get_sync_engine(
            build_url(
                os.getenv("PG_HOST", "localhost"),
                int(os.getenv("PG_PORT", "5432")),
                os.getenv("PG_DATABASE", "postgres"),
                os.getenv("PG_USER", "postgres"),
                os.getenv("PG_PASSWORD", ""),
                async_driver=False,
            )
        )
    Session = get_sync_session_factory(engine)
    client = TorrentioClient()
    session = Session()
    try:
        rows = _catalog_rows(session, refresh_days, batch_size)
        for kind, imdb_id, catalog_id in rows:
            try:
                if kind == "movie":
                    streams = client.get_streams(imdb_id, content_type="movie")
                else:
                    # Para series se usa un episodio de muestra (S1E1) como
                    # clasificacion del titulo completo.
                    streams = client.get_streams(
                        imdb_id, content_type="series", season=1, episode=1
                    )
                classification = classify_streams(streams)
                stats["checked"] += 1
                if classification.has_torrent:
                    stats["with_torrent"] += 1
                if not dry_run:
                    if kind == "movie":
                        _apply_movie(session, catalog_id, classification)
                    else:
                        _apply_series(session, catalog_id, classification)
                    session.commit()
            except Exception as exc:
                stats["errors"] += 1
                logger.warning("Error clasificando %s %s: %s", kind, imdb_id, exc)
                session.rollback()
            time.sleep(0.25)  # respeto a Torrentio/Cloudflare
    finally:
        session.close()

    stats["duration_seconds"] = round(time.time() - start, 1)
    return stats


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clasifica el catalogo con Torrentio")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument(
        "--refresh-days",
        type=int,
        default=7,
        help="Dias desde la ultima consulta para re-clasificar (default: 7)",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()
    stats = run_classify(
        batch_size=args.batch_size,
        refresh_days=args.refresh_days,
        dry_run=args.dry_run,
    )
    print(f"checked={stats['checked']}")
    print(f"with_torrent={stats['with_torrent']}")
    print(f"errors={stats['errors']}")
    print(f"duration_seconds={stats['duration_seconds']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
